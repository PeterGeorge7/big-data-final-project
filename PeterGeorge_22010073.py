import numpy as np
import sympy as sp


def optimize(
    obj_fn,
    method="calculus_based_opt",
    n_vars=1,
    minimize=True,
    constraints=None,
    inequality=False,
    initial_guess=None,
    epsilon=0.001,
    descent=True,
    epochs=10,
):

    # Define variables
    vars = sp.symbols(f"x_0:{n_vars}")
    f = sp.sympify(obj_fn)

    if method == "calculus_based_opt":
        grad = [sp.diff(f, v) for v in vars]
        critical_points = sp.solve(grad, vars, dict=True)

        if not critical_points:
            return None, None

        hessian = sp.hessian(f, vars)

        best_point = None
        best_value = None

        for point in critical_points:
            value = f.subs(point)
            hess_val = hessian.subs(point)

            eigenvals = hess_val.eigenvals()
            is_min = all(ev > 0 for ev in eigenvals)
            is_max = all(ev < 0 for ev in eigenvals)

            if (minimize and is_min) or (not minimize and is_max):
                best_point = [float(point[v]) for v in vars]
                best_value = float(value)

        return [round(x, 4) for x in best_point], round(best_value, 4)

    # ===============================
    # 2. Lagrange Optimization
    # ===============================
    elif method == "lagrange":
        if not constraints:
            raise ValueError("Constraints required for Lagrange method")

        lambdas = sp.symbols(f"lambda_0:{len(constraints)}")
        lagrangian = f

        for l, c in zip(lambdas, constraints):
            lagrangian += l * sp.sympify(c)

        equations = []
        for v in vars:
            equations.append(sp.diff(lagrangian, v))

        for l in lambdas:
            equations.append(sp.diff(lagrangian, l))

        solutions = sp.solve(equations, (*vars, *lambdas), dict=True)

        best_solution = solutions[0]
        best_point = [float(best_solution[v]) for v in vars]
        best_value = float(f.subs(best_solution))

        return [round(x, 4) for x in best_point], round(best_value, 4)

    # ===============================
    # 3. Newton's Method
    # ===============================
    elif method == "newton":
        if initial_guess is None:
            raise ValueError("Initial guess required for Newton method")

        grad = sp.Matrix([sp.diff(f, v) for v in vars])
        hessian = sp.hessian(f, vars)

        x = sp.Matrix(initial_guess)

        for _ in range(epochs):
            grad_val = grad.subs(dict(zip(vars, x)))
            hess_val = hessian.subs(dict(zip(vars, x)))

            grad_np = np.array(grad_val, dtype=float).reshape(-1, 1)
            hess_np = np.array(hess_val, dtype=float)

            if np.linalg.norm(grad_np) < epsilon:
                break

            x = x - sp.Matrix(np.linalg.inv(hess_np).dot(grad_np))

        best_point = [float(val) for val in x]
        best_value = float(f.subs(dict(zip(vars, x))))

        return [round(x, 4) for x in best_point], round(best_value, 4)

    # ===============================
    # 4. Steepest Descent / Ascent
    # ===============================
    elif method == "steepest":
        if initial_guess is None:
            raise ValueError("Initial guess required for Steepest method")

        grad = sp.Matrix([sp.diff(f, v) for v in vars])
        x = np.array(initial_guess, dtype=float)

        alpha = epsilon
        direction = -1 if descent else 1

        for _ in range(epochs):
            grad_val = grad.subs(dict(zip(vars, x)))
            grad_np = np.array(grad_val, dtype=float)
            x = x + direction * alpha * grad_np

        best_point = list(x)
        best_value = float(f.subs(dict(zip(vars, x))))

        return [round(x, 4) for x in best_point], round(best_value, 4)

    else:
        raise ValueError("Invalid optimization method")


if __name__ == "__main__":
    # Test 1: Calculus-based optimization
    result1 = optimize(
        "x_0**3 - x_0", method="calculus_based_opt", n_vars=1, minimize=True
    )
    print("Test 1:", result1)
    print("Expected: ([0.5774], -0.3849)")
    print()

    # Test 2: Lagrange method
    result2 = optimize(
        "2 * x_0 + x_1 + 10",
        method="lagrange",
        n_vars=2,
        minimize=False,
        constraints=["x_0 + 2 * x_1**2 - 3"],
    )
    print("Test 2:", result2)
    print("Expected: ([2.9688, 0.125], 16.0625)")
    print()

    # Test 3: Newton's method
    result3 = optimize(
        "2 * sin(x_0) - 0.1 * x_0**2",
        method="newton",
        n_vars=1,
        initial_guess=[2.5],
        epsilon=0.05,
    )
    print("Test 3:", result3)
    print("Expected: ([1.4276], 1.7757)")
    print()

# Test 4: Steepest descent
result4 = optimize(
    "x_0 - x_1 + 2 * x_0 ** 2 + 2 * x_0 * x_1 + x_1 ** 2",
    method="steepest",
    n_vars=2,
    initial_guess=[0, 0],
    descent=True,
    epochs=2,
)
print("Test 4:", result4)
print("Expected: ([-0.8, 1.2], -1.2)")
