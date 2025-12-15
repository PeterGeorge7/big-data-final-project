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
    """
    Unified optimization function that supports multiple optimization methods.

    Parameters:
        obj_fn (str): Objective function in variables x_0, x_1, and so on.
                     Example: "(x_0 - 2)**2 + (x_1 - 3)**2"
        method (str): Optimization method to use. Options:
                     - 'calculus_based_opt': Symbolic optimization using gradient and Hessian (unconstrained)
                     - 'lagrange': Lagrange method for constrained optimization
                     - 'newton': Newton's method for numerical unconstrained optimization
                     - 'steepest': Steepest descent/ascent method
        n_vars (int): Number of variables, default is 1
        minimize (bool): True for minimization, False for maximization, default is True
        constraints (list of str, optional): Constraint(s) for Lagrange method.
                                            Example: ['2 * x_0 + x_1 - 4']
        inequality (bool): True for inequality constraints, False for equality constraints (Lagrange only)
        initial_guess (list of floats, optional): Initial point for Newton and Steepest methods
        epsilon (float): Error threshold for Newton method, default is 0.001
        descent (bool): True for minimization, False for maximization (Steepest only), default is True
        epochs (int): Number of iterations for Steepest method, default is 10

    Returns:
        tuple: ([best_point], best_value) rounded to 4 decimal points
    """

    # Create symbolic variables
    if n_vars == 1:
        x = [sp.Symbol("x_0")]
    else:
        x = list(sp.symbols(" ".join([f"x_{var_num}" for var_num in range(n_vars)])))

    # Parse objective function
    f = sp.sympify(obj_fn)

    if method == "calculus_based_opt":
        return calculus_based_optimization(f, x, minimize)

    elif method == "lagrange":
        return lagrange_method(f, x, constraints, minimize)

    elif method == "newton":
        return newton_method(f, x, initial_guess, epsilon, minimize)

    elif method == "steepest":
        return steepest_method(f, x, initial_guess, descent, epochs)

    else:
        raise ValueError(f"method is unkown")


def calculus_based_optimization(f, x, minimize):
    """Calculus-based optimization using gradient and Hessian"""
    n_vars = len(x)
    gradient = [sp.diff(f, xi) for xi in x]
    critical_points = sp.solve(gradient, x, dict=True)
    hessian = sp.Matrix([[sp.diff(f, xi, xj) for xj in x] for xi in x])

    best_point = None
    best_value = None

    for point in critical_points:
        H = hessian.subs(point)

        try:
            if n_vars == 1:
                h_val = float(H[0, 0])
                if (minimize and h_val > 0) or (not minimize and h_val < 0):
                    value = float(f.subs(point))
                    if (
                        best_value is None
                        or (minimize and value < best_value)
                        or (not minimize and value > best_value)
                    ):
                        best_value = value
                        best_point = [float(point[x[0]])]
            else:
                hessian_eigenvalues = H.eigenvals()

                all_positive = all(
                    float(sp.re(ev)) > 0 for ev in hessian_eigenvalues.keys()
                )
                all_negative = all(
                    float(sp.re(ev)) < 0 for ev in hessian_eigenvalues.keys()
                )

                if (minimize and all_positive) or (not minimize and all_negative):
                    value = float(f.subs(point))
                    if (
                        best_value is None
                        or (minimize and value < best_value)
                        or (not minimize and value > best_value)
                    ):
                        best_value = value
                        best_point = [float(point[xi]) for xi in x]
        except Exception as e:
            continue

    best_point = [round(p, 4) for p in best_point]
    best_value = round(best_value, 4)

    return best_point, best_value


def lagrange_method(f, x, constraints, minimize):
    """Lagrange multiplier method for constrained optimization"""
    n_vars = len(x)
    n_constraints = len(constraints)

    if not minimize:
        f = -f

    if n_constraints == 1:
        lambdas = [sp.Symbol("lambda_0")]
    else:
        lambdas = list(
            sp.symbols(" ".join([f"lambda_{i}" for i in range(n_constraints)]))
        )

    g = [sp.sympify(constraint) for constraint in constraints]
    L = f + sum(lambdas[i] * g[i] for i in range(n_constraints))
    grad_L = [sp.diff(L, xi) for xi in x] + [sp.diff(L, lam) for lam in lambdas]
    lagrange_solutions = sp.solve(grad_L, x + lambdas, dict=True)

    best_point = None
    best_value = None

    for sol in lagrange_solutions:
        try:
            point = [float(sol[xi]) for xi in x]
            value = float(f.subs(sol))

            if not minimize:
                value = -value

            if (
                best_value is None
                or (minimize and value < best_value)
                or (not minimize and value > best_value)
            ):
                best_value = value
                best_point = point
        except:
            continue

    best_point = [round(optimal_point, 4) for optimal_point in best_point]
    best_value = round(best_value, 4)

    return best_point, best_value


def newton_method(f, x, initial_guess, epsilon, minimize):
    """Newton's method for numerical optimization"""
    n_vars = len(x)

    if not minimize:
        f = -f

    gradient = sp.Matrix([sp.diff(f, xi) for xi in x])
    hessian = sp.Matrix([[sp.diff(f, xi, xj) for xj in x] for xi in x])

    gradient_function = sp.lambdify(x, gradient, "numpy")
    compute_hessian = sp.lambdify(x, hessian, "numpy")
    f_fn = sp.lambdify(x, f, "numpy")

    point = np.array(initial_guess, dtype=float)

    max_iter = 100
    for _ in range(max_iter):
        if n_vars == 1:
            grad = np.array(gradient_function(point[0]), dtype=float).flatten()
        else:
            grad = np.array(gradient_function(*point), dtype=float).flatten()

        if np.linalg.norm(grad) < epsilon:
            break

        if n_vars == 1:
            hess = np.array(compute_hessian(point[0]), dtype=float).reshape(1, 1)
        else:
            hess = np.array(compute_hessian(*point), dtype=float)

        try:
            delta = np.linalg.solve(hess, -grad)
            point = point + delta
        except:
            break

    if n_vars == 1:
        value = float(f_fn(point[0]))
    else:
        value = float(f_fn(*point))

    if not minimize:
        value = -value

    best_point = [round(p, 4) for p in point.tolist()]
    best_value = round(value, 4)

    return best_point, best_value


def steepest_method(f, x, initial_guess, descent, epochs):
    """Steepest descent/ascent with exact line search (symbolic alpha)"""

    n_vars = len(x)

    grad_sym = [sp.diff(f, xi) for xi in x]

    current_point = list(initial_guess)

    for _ in range(epochs):
        subs_dict = dict(zip(x, current_point))
        grad_val = [float(g.subs(subs_dict)) for g in grad_sym]

        # Stop if gradient is (almost) zero
        if all(abs(gradient_component) < 1e-9 for gradient_component in grad_val):
            break

        # Direction
        if descent:
            direction = [-g for g in grad_val]
        else:
            direction = grad_val

        # Exact line search
        alpha = sp.symbols("alpha")
        next_point_expr = [
            current_point[i] + alpha * direction[i] for i in range(n_vars)
        ]

        objective_function_alpha = f.subs(dict(zip(x, next_point_expr)))
        d_phi = sp.diff(objective_function_alpha, alpha)

        sol = sp.solve(d_phi, alpha)
        real_solutions = [s for s in sol if s.is_real]

        if not real_solutions:
            break

        optimal_step_size = float(real_solutions[0])

        # Update point
        current_point = [
            current_point[i] + optimal_step_size * direction[i] for i in range(n_vars)
        ]

    final_subs = dict(zip(x, current_point))
    final_value = float(f.subs(final_subs))

    best_point = [round(p, 4) for p in current_point]
    best_value = round(final_value, 4)

    return best_point, best_value
